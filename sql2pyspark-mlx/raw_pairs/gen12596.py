from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount").alias("order_discount"))
)

result = (
    latest_match
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.when(F.col("city").rlike("^(Tokyo|Chiang Mai)$"), "CORE").when(F.col("city").rlike("^(Bangkok|New York)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("city").desc())
)
