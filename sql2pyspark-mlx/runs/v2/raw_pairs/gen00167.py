from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.city").isin(["Bangkok", "London", "Berlin"]))
    .select(
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
    .orderBy(F.col("cost").desc())
)
