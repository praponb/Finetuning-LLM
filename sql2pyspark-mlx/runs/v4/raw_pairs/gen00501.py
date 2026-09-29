from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city") == "Bangkok"))
    .select(
        "id",
        "supplier_name",
        "city",
        F.concat_ws(",", "supplier_name", "country").alias("supplier_name_country_key"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.month(F.col("ch.launch_date")).isin([1, 4, 9])), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand").isin(["Acme", "Initech", "Umbrella"])), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("city").isNotNull() & F.upper("supplier_name").like("%IAM%"))
    .withColumn("is_tokyo", F.when(F.col("city") == "Tokyo", 1).otherwise(0))
    .withColumn("clean_city", F.trim("city"))
)

result = (
    cleaned
    .select(
        "id",
        "supplier_name",
        "supplier_name_country_key",
        "is_tokyo",
        "clean_city",
        F.when(F.col("supplier_name").rlike("^(Lion City Imports|EuroGoods GmbH)$"), "GROUP_A").when(F.col("supplier_name").rlike("^(Siam Parts Co|Global Supply)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("supplier_name_zone"),
    )
    .orderBy(F.col("supplier_name_zone").asc_nulls_last(), "is_tokyo")
)
