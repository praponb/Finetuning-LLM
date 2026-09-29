from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Umbrella"]))
    .select("id", "stock_qty", "category")
    .unionByName(
        df_products
        .filter(F.array_contains("tags", "sale"))
        .select("id", "stock_qty", "category")
    )
    .distinct()
)
