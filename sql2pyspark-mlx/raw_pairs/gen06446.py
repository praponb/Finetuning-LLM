from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w_part = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_last())
w3 = Window.partitionBy("region").orderBy(F.col("event_seq").desc_nulls_last(), "id")

valid = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-02-17").cast("date"))
    .withColumn("city_group", F.when(F.col("city").isin(["New York", "Chiang Mai"]), "Group A").otherwise("Other"))
)

enriched = (
    valid
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "city_group",
        F.row_number().over(w).alias("event_seq"),
        F.dense_rank().over(w_part).alias("sqft_dense_rank"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

valid_stores = (
    deduped
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-04-04"))) & (~F.col("region").eqNullSafe(F.lit("Central"))))
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "sqft_dense_rank",
        F.col("event_seq").cast("double").alias("event_seq_str"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_filled"),
    )
)

result = (
    valid_stores
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "sqft_dense_rank",
        "region_filled",
        F.when(F.col("region").rlike("^(Central|North)$"), "GROUP_A").when(F.col("region").rlike("^(South|East)$"), "EXTENDED").otherwise("GROUP_B").alias("region_zone"),
    )
)
