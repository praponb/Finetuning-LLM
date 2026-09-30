from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .select("launch_date")
    .unionByName(
        df_products
        .filter(F.month("launch_date").isin([1, 2, 8]))
        .select("launch_date")
    )
    .orderBy("launch_date")
)
