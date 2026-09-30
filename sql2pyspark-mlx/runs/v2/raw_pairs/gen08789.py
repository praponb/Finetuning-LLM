from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Tokyo", "Bangkok"]) & F.col("supplier_name").like("Gl%") & F.col("country").isin(["DE", "SG"]))
    .groupBy("city")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
    .filter(F.col("rating_range") > 37)
    .orderBy(F.col("uk_count").desc(), F.col("rating_range").desc())
)
