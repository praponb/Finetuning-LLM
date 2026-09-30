from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "Chiang Mai"]) & ~F.col("country").isin(["JP", "TH"]))
    .select(
        "id",
        "city",
        "supplier_name",
        F.initcap(F.trim("city")).alias("city_title"),
    )
    .orderBy(F.col("city_title").desc(), F.col("supplier_name").desc(), "id")
    .limit(10)
)
