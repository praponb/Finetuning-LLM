from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Electronics")
    .select("id", "category")
    .intersect(
        df_products
        .filter(F.col("category").isin(["Fashion", "Grocery"]))
        .select("id", "category")
    )
)
