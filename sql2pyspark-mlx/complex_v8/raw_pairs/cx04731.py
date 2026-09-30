from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product").orderBy(F.col("premium").desc(), F.col("id").desc())

base_policys = (
    df_policies
    .filter(F.col("end_date") < F.lit("2026-04-25").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

latest_per_key = (
    base_policys
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

screened = (
    latest_per_key.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status").isNotNull()), "left_semi")
)

grouped = (
    screened
    .filter(F.col("start_date").between(F.lit("2024-03-27").cast("date"), F.lit("2025-03-23").cast("date")))
    .groupBy("product")
    .agg(
        F.count("*").alias("cnt"),
        F.max("premium").alias("max_premium"),
        F.datediff(F.max("end_date"), F.min("end_date")).alias("end_date_span_days"),
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
        F.round(F.sum("premium"), 2).alias("total_premium"),
    )
)

result = (
    grouped
    .groupingSets(
        [["cnt", "product"], ["cnt"], ["product"], []],
        "cnt", "product",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.min("max_premium").alias("min_max_premium"),
        F.round(F.sum("max_premium"), 2).alias("total_max_premium"),
    )
    .orderBy(F.col("cnt").asc_nulls_last(), F.col("product").asc_nulls_last())
)
