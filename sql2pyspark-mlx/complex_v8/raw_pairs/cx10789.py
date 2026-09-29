from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("city").orderBy(F.col("opened_on").desc(), F.col("id").desc())

prepared = (
    df_warehouses
    .filter((F.col("city") == "Singapore") & (F.col("opened_on") < F.lit("2025-07-03").cast("date")))
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
    )
)

deduped = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        F.date_format(F.col("opened_on"), "yyyy-MM").alias("opened_on_label"),
    )
    .orderBy("id")
)
