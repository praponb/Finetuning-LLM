from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_end_date_month_by = Window.partitionBy("holder_id", F.month("end_date")).orderBy(F.col("start_date").desc(), "id")
w_holder_id_end_date_month_by_2 = Window.partitionBy("holder_id", F.month("end_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_end_date_month_by_3 = Window.partitionBy("holder_id", F.month("end_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_by_start_date_prior = Window.partitionBy("product").orderBy(F.col("start_date").desc(), "id").rowsBetween(-1, -1)
w_product_by_start_date_ties = Window.partitionBy("product").orderBy(F.col("start_date").desc())

scoped_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat_ws("-", F.col("status"), F.col("product")).alias("status_product_label"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
)

filtered = (
    scoped_policys
    .crossJoin(scoped_policys.agg(F.max("premium").alias("overall_max_premium")))
    .filter(F.col("premium") < F.col("overall_max_premium"))
    .drop("overall_max_premium")
)

windowed = (
    filtered
    .select(
        "*",
        F.ntile(5).over(w_holder_id_end_date_month_by).alias("start_date_quintile"),
        F.lead("premium").over(w_holder_id_end_date_month_by).alias("next_premium"),
        F.min("premium").over(w_holder_id_end_date_month_by_2).alias("min_premium_so_far"),
        F.last("end_date").over(w_holder_id_end_date_month_by_3).alias("last_end_date"),
        F.sum("premium").over(w_product_by_start_date_prior).alias("premium_sum_1_prior"),
        F.round(F.percent_rank().over(w_product_by_start_date_ties), 3).alias("start_date_percent_rank"),
    )
)

result = (
    windowed
    .rollup("product", "start_date_quintile")
    .agg(
        F.grouping("product").alias("is_total_product"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.countDistinct("product").alias("distinct_products"),
    )
)
