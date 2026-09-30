from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Tokyo")))
    .withColumn("clean_supplier_name", F.trim("supplier_name"))
    .withColumn("country_title", F.initcap(F.trim("country")))
)

latest = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "supplier_name", "country", "city", "country_title")
)

valid = (
    latest
    .filter(F.col("city").isNotNull())
    .select("*", F.trim("country").alias("clean_country"))
)

eligible = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") > "2025-02-01"), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.city",
        "s.country_title",
        "s.clean_country",
        F.col("c.brand"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "clean_country",
        "brand",
        F.when(F.col("country").rlike("^(DE|SG)$"), "CORE").when(F.col("country").rlike("^(TH|US)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("country_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
    .limit(10)
)
