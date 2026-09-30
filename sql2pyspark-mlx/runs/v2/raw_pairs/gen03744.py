from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Books")
    .select("supplier_id")
    .intersect(
        df_products
        .filter(F.array_contains("tags", "sale"))
        .select("supplier_id")
    )
)
