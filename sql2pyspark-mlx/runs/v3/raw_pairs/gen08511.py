from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([1, 9, 10]))
    .select("id", "category")
    .intersect(
        df_products
        .filter(F.col("category") == "Electronics")
        .select("id", "category")
    )
)
