from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.length("supplier_name") < 7) & ((F.col("country") == "JP") | F.col("supplier_name").like("Eu%")))
    .select(
        "id",
        "supplier_name",
        "rating",
        F.initcap(F.trim("city")).alias("city_title"),
    )
    .orderBy("id")
    .limit(10)
)
