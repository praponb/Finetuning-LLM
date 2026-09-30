from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("supplier_id", "country").orderBy("rating", "id")
w5 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("launch_date", "id")

base = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-09-05").cast("date")) & F.col("supplier_id").isNotNull())
    .withColumn("price_whole", F.ceil("price"))
)

joined = (
    base
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.price",
        "s.cost",
        "s.launch_date",
        "s.supplier_id",
        F.col("d.city"),
        F.col("d.rating"),
        F.col("d.country"),
    )
)

scored = (
    joined
    .select(
        "id",
        "product_name",
        "category",
        "launch_date",
        "supplier_id",
        "city",
        "rating",
        F.ntile(3).over(w).alias("cost_bucket"),
        F.rank().over(w2).alias("rating_rank"),
        F.corr("cost", "price").over(w3).alias("corr_cost_price"),
        F.lead("rating", 2).over(w4).alias("next_rating"),
        F.sum("cost").over(w5).alias("running_cost"),
    )
)

valid = (
    scored
    .filter((F.col("launch_date") > F.lit("2023-02-28").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "launch_date",
        "supplier_id",
        "city",
        "rating",
        "cost_bucket",
        "rating_rank",
        "corr_cost_price",
        "next_rating",
        "running_cost",
        (F.col("next_rating") / F.nullif("corr_cost_price", F.lit(0))).alias("next_rating_per_corr_cost_price"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "product_name",
        "category",
        "rating",
        "cost_bucket",
        "corr_cost_price",
    )
)

daily = (
    deduped
    .groupBy("category")
    .agg(
        F.percentile_approx("rating", 0.9).alias("rating_p90"),
        F.countDistinct("category").alias("distinct_categories"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
)

result = (
    daily
    .select(
        "category",
        "distinct_categories",
        "num_categories",
        F.when(F.col("category").rlike("^(Books|Grocery)$"), "GROUP_B").when(F.col("category").rlike("^(Electronics|Home)$"), "GROUP_A").otherwise("PRIORITY").alias("category_zone"),
    )
    .orderBy("category_zone")
)
