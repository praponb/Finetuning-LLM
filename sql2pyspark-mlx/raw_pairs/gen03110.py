from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") != "DE") & ~F.col("country").isin(["UK", "DE", "TH"]) & (F.col("city") == "London"))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
    .withColumn("supplier_name_filled", F.coalesce("supplier_name", F.lit("unassigned")))
    .orderBy("city", "country")
)
