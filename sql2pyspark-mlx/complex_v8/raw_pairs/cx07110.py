from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status").orderBy(F.col("status_len").desc(), F.col("id").desc())

prepared_policys = (
    df_policies
    .filter(F.col("premium") > 2013.8)
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

latest = (
    prepared_policys
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .rollup("holder_id", "status", "status_len")
    .agg(
        F.max("status_len").alias("max_status_len"),
        F.sum(F.when(F.col("product") != "HOME", F.col("status_len")).otherwise(0)).alias("status_len_product_home"),
        F.round(F.avg("status_len"), 2).alias("avg_status_len"),
    )
)
