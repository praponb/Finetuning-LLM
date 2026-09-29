from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"), F.col("c.price").alias("product_price"))
)

result = (
    with_last
    .select(
        "id",
        "country",
        "city",
        "rating",
        "launch_date",
        F.when(F.col("city").rlike("^(Bangkok|Berlin)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "GROUP_B").otherwise("CORE").alias("city_zone"),
    )
)
