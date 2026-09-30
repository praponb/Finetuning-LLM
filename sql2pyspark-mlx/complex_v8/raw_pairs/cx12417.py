from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_by_end_dat = Window.partitionBy("holder_id", "product").orderBy(F.col("end_date").desc())
w_end_date_month_status_by_end = Window.partitionBy(F.month("end_date"), "status").orderBy(F.col("end_date").desc(), "id")

filtered = (
    df_policies
    .filter((F.col("status").isNotNull()) & (F.col("premium").between(2013.8, 3544.1)))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "status_prefix",
        "end_date_label",
        F.rank().over(w_holder_id_product_by_end_dat).alias("end_date_rank"),
        F.rank().over(w_end_date_month_status_by_end).alias("end_date_rank_2"),
    )
)

tagged = (
    metrics
    .filter(F.col("status_prefix").isNotNull())
    .withColumn("premium_per_end_date_rank_2", F.round(F.col("premium") / F.nullif(F.col("end_date_rank_2"), F.lit(0)), 3))
)

result = (
    tagged
    .select(
        "id",
        "premium",
        "status_prefix",
        "end_date_label",
        "end_date_rank",
        "end_date_rank_2",
        "premium_per_end_date_rank_2",
        F.lower(F.trim(F.col("status_prefix"))).alias("status_prefix_clean"),
        F.date_trunc("year", F.col("start_date")).alias("start_date_year"),
    )
)
