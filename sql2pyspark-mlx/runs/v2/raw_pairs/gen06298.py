from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.upper(F.trim("supplier_name")) == "SIAM PARTS CO") & F.col("city").isin(["Bangkok", "Singapore"]))
    .select("id", "rating")
    .orderBy(F.col("id").desc())
)
