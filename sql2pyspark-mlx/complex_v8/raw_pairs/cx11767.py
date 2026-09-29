from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc(), F.col("id").desc())

cleaned_policys = (
    df_policies
    .filter((F.col("holder_id") <= 9) & (F.col("start_date") < F.lit("2025-02-13").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.concat_ws("|", F.col("status"), F.col("product")).alias("status_product_label"),
        F.year(F.col("end_date")).alias("end_date_year"),
        F.add_months(F.col("end_date"), 1).alias("end_date_plus_1m"),
    )
)

deduped = (
    cleaned_policys
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

product_stats = (
    deduped
    .groupBy("product")
    .agg(F.avg("end_date_year").alias("avg_end_date_year"))
)

compared = (
    deduped.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.end_date_year") > F.col("g.avg_end_date_year"))
    .select(
        "s.*",
        "g.avg_end_date_year",
        F.round(F.col("s.end_date_year") - F.col("g.avg_end_date_year"), 2).alias("end_date_year_minus_avg"),
    )
)

latest = (
    compared
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .rollup("product", "end_date_year")
    .agg(
        F.count(F.when(F.col("product").isin("TRAVEL", "HEALTH", "LIFE"), 1)).alias("n_product_in_travel_health_lif"),
        F.min("avg_end_date_year").alias("min_avg_end_date_year"),
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("end_date_year").asc_nulls_last(),
    )
)
