from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_premium_tier_by_premi = Window.partitionBy("status", "premium_tier").orderBy(F.col("premium").desc(), "id")
w_holder_id_status = Window.partitionBy("holder_id", "status")
w_product_by_days_since_start = Window.partitionBy("product").orderBy(F.col("days_since_start_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_holder_id_by_premium_full = Window.partitionBy("holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_premium_prior_6 = Window.partitionBy("holder_id").orderBy("premium", "id").rowsBetween(-6, -1)
w_holder_id_by_premium_ties = Window.partitionBy("holder_id").orderBy("premium")
w_status_premium_tier_by_premi_2 = Window.partitionBy("status", "premium_tier").orderBy(F.col("premium").desc())
w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_premium_tier_by_premi_3 = Window.partitionBy("status", "premium_tier").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_by_days_since_start_2 = Window.partitionBy("product").orderBy(F.col("days_since_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_by_days_since_start_3 = Window.partitionBy("product").orderBy(F.col("days_since_start_date").desc())
w_status_premium_tier_by_premi_4 = Window.partitionBy("status", "premium_tier").orderBy(F.col("premium").desc(), "id").rowsBetween(-3, -1)

prepared = (
    df_policies
    .filter((F.col("premium") <= 3521.8) & (F.col("product").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.when(F.col("premium") >= 2945.9, "HIGH").when(F.col("premium") >= 1132.1, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

sequenced = (
    prepared
    .select(
        "*",
        F.lead("days_since_start_date").over(w_status_premium_tier_by_premi).alias("next_days_since_start_date"),
        F.sum("days_since_start_date").over(w_holder_id_status).alias("holder_id_status_sum_days_since_start_date"),
        F.round(F.avg("premium").over(w_product_by_days_since_start), 2).alias("premium_avg_3"),
        F.last("premium").over(w_holder_id_by_premium_full).alias("last_premium"),
        F.max("days_since_start_date").over(w_holder_id_by_premium_prior_6).alias("days_since_start_date_max_6_prior"),
        F.lag("days_since_start_date", 2).over(w_status_premium_tier_by_premi).alias("prev2_days_since_start_date"),
        F.round(F.percent_rank().over(w_holder_id_by_premium_ties), 3).alias("premium_percent_rank"),
        F.dense_rank().over(w_status_premium_tier_by_premi_2).alias("premium_dense_rank"),
        F.last("premium", ignorenulls=True).over(w_holder_id_by_premium_running).alias("last_known_premium"),
        F.sum("premium").over(w_status_premium_tier_by_premi_3).alias("running_premium"),
        (F.col("premium") - F.lag("premium").over(w_status_premium_tier_by_premi)).alias("premium_change"),
        F.lead("premium").over(w_status_premium_tier_by_premi).alias("next_premium"),
        F.nth_value("premium", 2).over(w_product_by_days_since_start_2).alias("second_premium"),
        F.rank().over(w_product_by_days_since_start_3).alias("days_since_start_date_rank"),
        F.round(F.avg("premium").over(w_status_premium_tier_by_premi_4), 2).alias("premium_avg_3_prior"),
    )
)

top_rows = (
    sequenced
    .filter(F.col("premium_dense_rank") == 1)
    .drop("premium_dense_rank")
)

result = (
    top_rows
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "days_since_start_date",
        "premium_tier",
        "next_days_since_start_date",
        "last_premium",
        "days_since_start_date_max_6_prior",
        "prev2_days_since_start_date",
        "premium_percent_rank",
        "last_known_premium",
        "premium_change",
        "next_premium",
        "days_since_start_date_rank",
        "premium_avg_3_prior",
    )
    .orderBy(
        F.col("next_premium").desc(),
        F.col("prev2_days_since_start_date").desc(),
    )
)
