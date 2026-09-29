from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.length("supplier_name") < 10)
    .select(
        "*",
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
    )
)

result = (
    base
    .select("city", "id")
    .intersect(
        df_suppliers
        .filter(F.col("rating") >= 3.0)
        .select("city", "id")
    )
)
