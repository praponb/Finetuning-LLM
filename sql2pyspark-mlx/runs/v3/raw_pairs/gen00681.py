from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Corporate"]))
    .select("country")
    .intersect(
        df_customers
        .filter(F.col("city").isin(["Berlin", "Chiang Mai"]))
        .select("country")
    )
)
