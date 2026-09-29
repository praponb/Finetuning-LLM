from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Fashion") & (F.col("category") != "Grocery"))
    .select("cost", F.round((F.col("cost") * 1.1), 2).alias("adjusted_cost"))
    .orderBy(F.col("adjusted_cost").desc(), F.col("cost").desc())
)
