from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["SMB", "Corporate", "Consumer"]))
    .select("id", "segment")
    .unionByName(
        df_customers
        .filter(~F.col("segment").isin(["SMB", "Corporate", "Consumer"]))
        .select("id", "segment")
    )
    .orderBy("segment")
)
