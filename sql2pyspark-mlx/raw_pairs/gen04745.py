from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") > 163)
    .select(
        "id",
        "last_restock",
        (F.datediff(F.current_date(), "last_restock") / 365).alias("years_since_last_restock"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
