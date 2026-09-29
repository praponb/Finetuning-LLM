from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"), F.col("c.sale_date"))
)

result = (
    with_latest
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.when(F.col("city").rlike("^(Chiang Mai|Berlin)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|New York)$"), "GROUP_B").otherwise("PRIORITY").alias("city_zone"),
    )
)
