from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "Bangkok", "New York"]))
    .select(
        "rating",
        "supplier_name",
        "country",
        F.initcap(F.trim("country")).alias("country_title"),
    )
)
