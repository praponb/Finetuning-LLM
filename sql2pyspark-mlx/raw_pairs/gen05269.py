from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("created_at"))
w2 = Window.partitionBy("customer_id", "code").orderBy(F.col("product_id").desc_nulls_first())
w_recent = Window.partitionBy("customer_id").orderBy("price")
w_hist = Window.partitionBy("code").orderBy(F.col("product_id").desc())
w5 = Window.partitionBy("code").orderBy(F.col("product_id").desc_nulls_first())

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

flattened = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "product_id",
        "price",
        F.explode("coupon_codes").alias("code"),
    )
)

with_windows = (
    flattened
    .filter(F.hour("created_at").between(10, 18))
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w)), 4))
    .withColumn("product_id_percent_rank", F.percent_rank().over(w2))
    .withColumn("price_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("product_id_rank", F.rank().over(w_hist))
    .withColumn("product_id_cume_dist", F.cume_dist().over(w5))
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        "a.customer_id",
        "a.product_id",
        "a.price",
        "a.code",
        "a.product_id_rank",
        F.col("c.credit_limit"),
        F.col("c.segment"),
    )
)

with_dims_carts = (
    with_dims
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.city"), F.col("b.country"), F.col("b.age"))
)

result = (
    with_dims_carts
    .select(
        "country",
        F.col("price").alias("PRICE"),
        F.col("credit_limit").alias("CREDIT_LIMIT_VAL"),
    )
    .unpivot(
        ids=["country"],
        values=["PRICE", "CREDIT_LIMIT_VAL"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
)
