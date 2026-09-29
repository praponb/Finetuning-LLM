from pyspark.sql import functions as F

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.size(F.col("x.tags")) > 1), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "city",
        "rating",
        F.when(F.col("city").rlike("^(New York|Chiang Mai)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|Singapore)$"), "EXTENDED").otherwise("GROUP_A").alias("city_zone"),
    )
)
