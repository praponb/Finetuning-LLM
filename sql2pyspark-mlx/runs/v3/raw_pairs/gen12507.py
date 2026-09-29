from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(62, 64))
    .select("quantity")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") <= "2023-03-01")
        .select("quantity")
    )
    .orderBy("quantity")
)
