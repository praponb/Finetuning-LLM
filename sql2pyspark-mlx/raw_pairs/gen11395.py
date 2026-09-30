from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee"))
)

result = (
    with_latest
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "shipping_fee",
        F.when(F.col("city").rlike("^(New York|Tokyo)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_B").otherwise("EXTENDED").alias("city_zone"),
    )
)
