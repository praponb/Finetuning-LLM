from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(F.col("rating").between(2.8, 4.1))
    .withColumn("country_title", F.initcap(F.trim("country")))
    .withColumn("supplier_name_country", F.concat("supplier_name", F.lit(", "), "country"))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") >= 320), "left_semi")
)

latest = (
    not_flagged
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "supplier_name", "city", "country_title")
)

latest_match = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.supplier_name", F.col("c.brand"), F.col("c.category"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.size(F.col("ch.tags")) > 2), "left_semi")
)

kept_suppliers = (
    kept
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand") != "Stark"), "left_anti")
)

kept_suppliers_2 = (
    kept_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") >= 950), "left_anti")
)

result = (
    kept_suppliers_2
    .select(
        "id",
        "brand",
        F.when(F.col("brand").rlike("^(Initech|Umbrella)$"), "CORE").otherwise("OTHER_ZONE").alias("brand_zone"),
    )
    .orderBy(F.col("brand").asc_nulls_last())
)
