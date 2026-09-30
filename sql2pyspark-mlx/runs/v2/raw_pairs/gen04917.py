from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "US", "SG"]) & F.col("city").isin(["Berlin", "London", "Bangkok"]) & ~F.col("country").isin(["TH", "JP", "US"]))
    .select("rating", "city", F.initcap(F.trim("country")).alias("country_title"))
    .orderBy(F.col("city").desc())
)
