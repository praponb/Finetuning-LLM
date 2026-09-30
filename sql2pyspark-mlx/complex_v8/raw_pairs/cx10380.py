from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_all_by_end_date_ties = Window.orderBy("end_date")
w_product_holder_id_by_end_dat = Window.partitionBy("product", "holder_id").orderBy(F.col("end_date").desc(), "id")
w_product_end_date_year_by_pre = Window.partitionBy("product", F.year("end_date")).orderBy(F.col("premium").desc_nulls_first(), "id")
w_product_holder_id = Window.partitionBy("product", "holder_id")
w_product_holder_id_by_end_dat_2 = Window.partitionBy("product", "holder_id").orderBy(F.col("end_date").desc(), "id").rowsBetween(-6, -1)
w_all_by_premium_max_6_prior = Window.orderBy("premium_max_6_prior", "id")
w_all_by_premium_max_6_prior_r = Window.orderBy("premium_max_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next2_end_date_year = Window.partitionBy(F.year("next2_end_date"))

cleaned = (
    df_policies
    .filter(F.col("product") == "AUTO")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.round(F.percent_rank().over(w_product_by_premium), 3).alias("premium_percent_rank"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_product), 2).alias("premium_vs_holder_id_product_avg"),
        F.round(F.cume_dist().over(w_all_by_end_date_ties), 3).alias("end_date_cume_dist"),
        F.lead("end_date").over(w_product_holder_id_by_end_dat).alias("next_end_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_product), F.lit(0)), 4).alias("premium_share_of_holder_id_product"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_product_end_date_year_by_pre)).alias("days_since_prev_end_date"),
        F.count(F.lit(1)).over(w_product_holder_id).alias("rows_in_product_holder_id"),
        F.lead("end_date", 2).over(w_product_end_date_year_by_pre).alias("next2_end_date"),
        F.max("premium").over(w_product_holder_id_by_end_dat_2).alias("premium_max_6_prior"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.rank().over(w_all_by_premium_max_6_prior).alias("premium_max_6_prior_rank"),
        F.first("end_date", ignorenulls=True).over(w_all_by_premium_max_6_prior_r).alias("first_known_end_date"),
        F.round(F.col("premium_max_6_prior") / F.nullif(F.sum("premium_max_6_prior").over(w_next2_end_date_year), F.lit(0)), 4).alias("premium_max_6_prior_share_of_next2_end_date_year"),
    )
)

result = (
    metrics
    .filter(F.col("premium_vs_holder_id_product_avg") >= -1500.0)
    .select(
        "id",
        "premium",
        "end_date",
        "product_clean",
        "premium_percent_rank",
        "premium_vs_holder_id_product_avg",
        "end_date_cume_dist",
        "next_end_date",
        "premium_share_of_holder_id_product",
        "days_since_prev_end_date",
        "next2_end_date",
        "first_known_end_date",
        "premium_max_6_prior_share_of_next2_end_date_year",
    )
    .orderBy(
        F.col("first_known_end_date").desc_nulls_first(),
        F.col("premium").desc_nulls_first(),
    )
)
