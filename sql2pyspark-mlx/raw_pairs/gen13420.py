from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.supplier_name", "s.country", "s.city", F.col("c.launch_date"))
)

result = (
    latest_match
    .select(
        "id",
        "supplier_name",
        "city",
        F.when(F.col("city").rlike("^(New York|Tokyo)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy("city_zone", "city")
)
