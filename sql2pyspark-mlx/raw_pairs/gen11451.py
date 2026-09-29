from pyspark.sql import functions as F

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") <= 600), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("country").rlike("^(UK|TH)$"), "GROUP_B").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc_nulls_first())
)
