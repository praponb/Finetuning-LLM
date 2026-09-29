from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc_nulls_first(), "id")
w2 = Window.partitionBy("brand").orderBy(F.col("rn").desc(), "id")
w3 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("price").desc(), "id")
w5 = Window.partitionBy("brand", "country").orderBy("price", "id")
w6 = Window.partitionBy("brand", "supplier_id")
w7 = Window.orderBy("price", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("supplier_id", "brand").orderBy(F.col("price_avg_2_prior").desc(), "id")

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.upper("product_name").alias("product_name_upper"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

joined = (
    deduped
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.rating"), F.col("b.country"))
)

ranked = (
    joined
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

scored = (
    ranked
    .filter(F.col("rn") == 1)
    .select(
        "*",
        F.avg("cost").over(w3).alias("cost_avg_6"),
        F.ntile(3).over(w4).alias("price_bucket"),
        F.lead("rating", 1).over(w5).alias("next_rating"),
        F.round((F.col("price") / F.sum("price").over(w6)), 4).alias("price_share"),
        F.avg("price").over(w7).alias("price_avg_2_prior"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w8))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.quantity"))
)

latest = (
    with_latest
    .withColumn("rn_2", F.row_number().over(w9))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

result = (
    latest
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "rating",
        F.when(F.col("brand").rlike("^(Stark|Umbrella)$"), "GROUP_B").when(F.col("brand").rlike("^(Acme|Globex)$"), "EXTENDED").otherwise("PRIORITY").alias("brand_zone"),
    )
    .orderBy(F.col("category").desc())
)
