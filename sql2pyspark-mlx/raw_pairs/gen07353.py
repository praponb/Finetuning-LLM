from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty"))
)

result = (
    with_last
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.when(F.col("country").rlike("^(UK|SG)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
    .limit(5)
)
