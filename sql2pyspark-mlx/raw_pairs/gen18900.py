from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("cost").desc_nulls_last(), "id")

latest = (
    df_store_sales
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.cost"))
)

enriched_store_sales = (
    enriched
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.sale_date",
        "a.rank_in_group",
        "a.cost",
        F.col("d.product_name"),
    )
)

cleaned = (
    enriched_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-08-31").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "rank_in_group",
        "cost",
        "product_name",
        F.greatest("rank_in_group", "cost").alias("greatest_rank_in_group_cost"),
    )
)

latest_store_sales = (
    cleaned
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
    .select(
        "id",
        "store_id",
        "sale_date",
        "rank_in_group",
        "cost",
        "greatest_rank_in_group_cost",
    )
)

joined = (
    latest_store_sales
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.sale_date",
        "src.rank_in_group",
        "src.cost",
        "src.greatest_rank_in_group_cost",
        F.col("s.city"),
        F.col("s.sqft"),
    )
)

result = (
    joined
    .select("id", "store_id", "sale_date", "cost", "city", "sqft")
    .orderBy(F.col("sale_date").desc())
)
