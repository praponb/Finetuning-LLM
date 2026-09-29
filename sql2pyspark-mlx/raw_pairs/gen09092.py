from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("revenue_per_unit").asc_nulls_first())
w4 = Window.partitionBy("store_city").orderBy(F.col("revenue_per_unit_cume_dist").desc_nulls_first(), "id")
w5 = Window.partitionBy("store_city", "store_id").orderBy(F.col("revenue_per_unit_cume_dist").asc_nulls_last(), "id")

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-05-19").cast("date")) & (F.col("revenue") >= 1900))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
)

metrics = (
    base
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "revenue_per_unit",
        F.lag("revenue", 2).over(w).alias("prev_revenue"),
        F.first("revenue_per_unit", ignorenulls=True).over(w2).alias("first_known_revenue_per_unit"),
        F.cume_dist().over(w3).alias("revenue_per_unit_cume_dist"),
    )
)

joined = (
    metrics
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.city") == "New York")
    .select(
        "src.id",
        "src.store_id",
        "src.sale_date",
        "src.revenue_per_unit",
        "src.revenue_per_unit_cume_dist",
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("s.region"),
    )
)

with_dims = (
    joined
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.sale_date",
        "s.revenue_per_unit_cume_dist",
        "s.city",
        F.col("b.store_name").alias("store_store_name"),
        F.col("b.city").alias("store_city"),
    )
)

base_store_sales = (
    with_dims
    .filter(F.col("store_id").isin([1, 2, 4]))
    .select(
        "id",
        "store_id",
        "revenue_per_unit_cume_dist",
        "store_city",
        F.trim("store_store_name").alias("clean_store_store_name"),
        F.upper("city").alias("city_upper"),
        F.coalesce("store_city", F.lit("UNKNOWN")).alias("store_city_clean"),
    )
)

ranked = (
    base_store_sales
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select("id", "store_id", "clean_store_store_name", "store_city_clean", "row_num")
)
