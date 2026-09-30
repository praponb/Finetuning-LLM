from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("brand").orderBy("price", "id")
w2 = Window.partitionBy("category").orderBy("launch_date", "id")
w3 = Window.partitionBy("category", "brand").orderBy("launch_date", "id").rowsBetween(-5, -1)

cleaned = (
    df_products
    .filter(F.col("cost").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.regexp_replace("product_name", " ", "").alias("product_name_slug"),
        F.last_day("launch_date").alias("launch_date_month_end"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.lag("cost").over(w_part).alias("prev_cost"),
        F.lead("price").over(w_part).alias("next_price"),
        F.lag("price").over(w_part).alias("prev_price"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

metrics = (
    enriched
    .withColumn("next_seq_no", F.lead("seq_no").over(w2))
    .withColumn("next_prev_cost", F.lead("prev_cost").over(w2))
    .withColumn("seq_no_2", F.row_number().over(w2))
    .withColumn("prev_cost_avg_5_prior", F.avg("prev_cost").over(w3))
)

enriched_products = (
    metrics
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.product_name",
        "src.brand",
        "src.price",
        "src.launch_date",
        "src.tags",
        "src.supplier_id",
        "src.prev_price",
        "src.seq_no",
        "src.next_seq_no",
        "src.next_prev_cost",
        "src.prev_cost_avg_5_prior",
        F.col("p.supplier_name"),
        F.col("p.city"),
        F.col("p.country"),
    )
)

cleaned_products = (
    enriched_products
    .filter(F.col("brand").isNotNull())
    .select(
        "*",
        F.upper("city").alias("city_upper"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

joined = (
    cleaned_products
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.price",
        "src.tags",
        "src.supplier_id",
        "src.prev_price",
        "src.seq_no",
        "src.next_seq_no",
        "src.next_prev_cost",
        "src.city",
        F.col("p.city").alias("supplier_city"),
        F.col("p.supplier_name").alias("supplier_supplier_name"),
        F.col("p.country").alias("supplier_country"),
    )
)

result = (
    joined
    .groupBy("supplier_city", "supplier_id")
    .pivot("city", ["Singapore", "Tokyo", "London"])
    .agg(F.max("price").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
