from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "supplier_id").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("brand").orderBy("cost", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w_hist = Window.orderBy("revenue", "id")
w5 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-05-13").cast("date")) & (~F.col("brand").eqNullSafe(F.lit("Umbrella"))))
    .select("*", F.ceil("cost").alias("cost_whole"))
)

joined = (
    valid
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id").eqNullSafe(F.col("s.id")), "left")
    .select("e.*", F.col("s.country"), F.col("s.city"))
)

deduped = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "country",
    )
)

with_last = (
    latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.tags", F.col("c.revenue"))
)

result = (
    with_last
    .filter(F.array_contains("tags", "premium"))
    .select(
        "id",
        "tags",
        "revenue",
        F.row_number().over(w_hist).alias("row_seq"),
        F.lead("revenue", 1).over(w_hist).alias("next_revenue"),
        F.last("revenue", ignorenulls=True).over(w5).alias("last_known_revenue"),
    )
)
