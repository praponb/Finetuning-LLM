from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc_nulls_first(), "id")

deduped = (
    df_stores
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.when(F.col("city").rlike("^(Bangkok|Singapore)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "GROUP_B").otherwise("CORE").alias("city_zone"),
    )
    .orderBy(F.col("sqft").asc_nulls_last(), F.col("city").desc())
)
