from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "TH", "SG"]))
    .select(
        "rating",
        "city",
        "country",
        F.initcap(F.trim("country")).alias("country_title"),
    )
)
