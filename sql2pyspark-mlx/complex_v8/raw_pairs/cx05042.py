from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("product", "holder_id").orderBy("days_since_start_date", "id")
w_all_by_days_since_start_date = Window.orderBy("days_since_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product = Window.partitionBy("product")
w_all_by_days_since_start_date_2 = Window.orderBy("days_since_start_date", "id")
w_product_end_date_year_by_end = Window.partitionBy("product", F.year("end_date")).orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_end_date_year = Window.partitionBy(F.year("second_end_date"))
w_product_holder_id_by_end_dat = Window.partitionBy("product", "holder_id").orderBy("end_date_week")

scoped = (
    df_policies
    .filter((F.col("premium") < 4670.9) | (F.col("status") == "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

unique_rows = (
    scoped
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    unique_rows
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        "end_date_week",
        F.last("start_date", ignorenulls=True).over(w_all_by_days_since_start_date).alias("last_known_start_date"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.round(F.col("days_since_start_date") - F.avg("days_since_start_date").over(w_product), 2).alias("days_since_start_date_vs_product_avg"),
        F.rank().over(w_all_by_days_since_start_date_2).alias("days_since_start_date_rank"),
        F.min("days_since_start_date").over(w_product).alias("product_min_days_since_start_date"),
        F.nth_value("end_date", 2).over(w_product_end_date_year_by_end).alias("second_end_date"),
    )
)

enriched = (
    ranked
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "end_date_week",
        "last_known_start_date",
        "rows_in_product",
        "days_since_start_date_vs_product_avg",
        "product_min_days_since_start_date",
        "second_end_date",
        F.add_months(F.col("last_known_start_date"), 6).alias("last_known_start_date_plus_6m"),
        F.coalesce(F.col("rows_in_product"), F.lit(0)).alias("rows_in_product_filled"),
        F.date_trunc("quarter", F.col("last_known_start_date")).alias("last_known_start_date_quarter"),
    )
)

sequenced = (
    enriched
    .withColumn("second_end_date_year_min_rows_in_product_filled", F.min("rows_in_product_filled").over(w_second_end_date_year))
    .withColumn("end_date_week_cume_dist", F.round(F.cume_dist().over(w_product_holder_id_by_end_dat), 3))
)

screened = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") == "APPROVED"), "left_semi")
    .filter(F.col("holder_id").isNotNull())
)

result = (
    screened
    .filter(F.col("second_end_date_year_min_rows_in_product_filled") <= 150)
    .select(
        "id",
        "holder_id",
        "product",
        "end_date_week",
        "last_known_start_date",
        "rows_in_product",
        "days_since_start_date_vs_product_avg",
        "product_min_days_since_start_date",
        "last_known_start_date_plus_6m",
        "rows_in_product_filled",
        "last_known_start_date_quarter",
    )
    .orderBy(F.col("days_since_start_date_vs_product_avg").asc_nulls_last())
)
