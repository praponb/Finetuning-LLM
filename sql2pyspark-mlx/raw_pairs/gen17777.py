from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("last_restock_year").asc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock_year").desc_nulls_first())
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("units").alias("avg_store_sale_units"))

valid = (
    df_inventory
    .filter(F.col("quantity") < 130)
    .select("*", F.year("last_restock").alias("last_restock_year"))
)

matched_keys = (
    valid
    .select("product_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("last_restock") > "2025-03-01")
        .select("product_id", "id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.dense_rank().over(w).alias("last_restock_year_dense_rank"),
        F.rank().over(w2).alias("last_restock_year_rank"),
        F.rank().over(w2).alias("last_restock_year_rank_2"),
    )
)

filtered = (
    enriched
    .filter(F.col("last_restock") > F.lit("2023-07-14").cast("date"))
    .withColumn("greatest_last_restock_year_rank_quantity", F.greatest("last_restock_year_rank", "quantity"))
)

result = (
    filtered
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "last_restock_year",
        "last_restock_year_dense_rank",
        "last_restock_year_rank",
        "greatest_last_restock_year_rank_quantity",
        F.col("avg_store_sale_units"),
    )
    .orderBy(F.col("last_restock_year_dense_rank").desc())
)
