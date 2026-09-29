from pyspark.sql import functions as F

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost") >= 410), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("country").rlike("^(DE|JP)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "EXTENDED").otherwise("CORE").alias("country_zone"),
    )
)
