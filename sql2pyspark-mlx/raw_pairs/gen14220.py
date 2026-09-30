from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.city",
        "s.rating",
        F.col("c.launch_date"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("country").rlike("^(SG|JP)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "GROUP_B").otherwise("EXTENDED").alias("country_zone"),
    )
    .orderBy(F.col("city").asc_nulls_last())
)
