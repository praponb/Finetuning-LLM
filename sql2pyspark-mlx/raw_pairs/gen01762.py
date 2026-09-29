from pyspark.sql import functions as F

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category") != "Sports"), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("country").rlike("^(DE|JP)$"), "EXTENDED").when(F.col("country").rlike("^(TH|US)$"), "GROUP_A").otherwise("CORE").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc(), "id")
)
