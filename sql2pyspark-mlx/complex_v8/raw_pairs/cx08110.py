from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("region").orderBy(F.col("joined_on").desc(), F.col("id").desc())

cleaned_holders = (
    df_policyholders
    .filter((F.col("joined_on") < F.lit("2025-07-02").cast("date")) & (F.col("segment") == "RETAIL"))
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
    )
)

deduped = (
    cleaned_holders
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .groupingSets(
        [["region", "segment"], ["region"], ["segment"], []],
        "region", "segment",
    )
    .agg(
        F.grouping("region").alias("is_total_region"),
        F.count(F.when(F.length(F.trim(F.col("holder_name"))) >= 6, 1)).alias("n_length_trim_holder_name_6"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("region").asc_nulls_last(), F.col("segment").asc_nulls_last())
)
