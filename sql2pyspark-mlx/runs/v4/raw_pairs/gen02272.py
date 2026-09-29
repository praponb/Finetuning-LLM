from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter(F.col("region") == "Central")
    .withColumn("store_name_length", F.length("store_name"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

base = (
    with_latest
    .filter(~F.col("city").isin(["London", "Chiang Mai"]))
    .withColumn("sqft_minus_store_name_length", (F.col("sqft") - F.col("store_name_length")))
)

result = (
    base
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "store_name_length",
        "quantity",
        F.when(F.col("city").rlike("^(London|Berlin)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "CORE").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("sqft").desc_nulls_first())
)
