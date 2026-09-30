from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.opened_date").between("2024-06-01", "2024-09-03") | ~F.col("s.city").isin(["Berlin", "New York", "Bangkok"]))
    .select(
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("i.store_id"),
        F.col("i.quantity"),
    )
    .orderBy(F.col("quantity").desc())
)
