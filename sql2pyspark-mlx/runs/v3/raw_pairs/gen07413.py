from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") != "UK") & ~F.col("country").isin(["DE", "TH"]) & F.col("country").isin(["TH", "JP"]))
    .select("id", "city", "supplier_name", F.round("rating", 0).alias("rating_rounded"))
)
