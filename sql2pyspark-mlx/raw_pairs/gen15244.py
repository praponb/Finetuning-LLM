from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("amount", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("last7_max_amount").desc(), "id")

with_dims = (
    df_orders
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.order_date",
        "a.channel",
        "a.amount",
        F.col("p.region"),
        F.col("p.store_name"),
    )
)

enriched = (
    with_dims
    .select(
        "*",
        F.count("amount").over(w_grp).alias("last7_count_amount"),
        F.min("amount").over(w_grp).alias("last7_min_amount"),
        F.max("amount").over(w_grp).alias("last7_max_amount"),
        F.avg("amount").over(w_grp).alias("last7_avg_amount"),
    )
)

metrics = (
    enriched
    .filter(F.col("store_name").isNotNull())
    .withColumn("amount_bucket", F.ntile(10).over(w2))
    .withColumn("last7_max_amount_bucket", F.ntile(3).over(w3))
)

matched_keys = (
    metrics
    .select("store_id", "channel")
    .subtract(
        df_orders
        .filter(F.col("amount").isNotNull())
        .select("store_id", "channel")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("store_id", "amount", "last7_min_amount", "last7_avg_amount")
    .unpivot(
        ids=["store_id"],
        values=["amount", "last7_min_amount", "last7_avg_amount"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
