from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id").orderBy("prior_unit_price_1d", "id")

covered = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.unit_price").alias("prior_unit_price_1d"),
    )
)

deduped = (
    covered
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

per_group = (
    deduped
    .filter(F.col("prior_rows_1d").between(1, 8))
    .groupBy("sku_id", "prior_rows_1d")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.min("moved_at").alias("first_moved_at"),
        F.sum("prior_rows_1d").alias("total_prior_rows_1d"),
        F.max("prior_unit_price_1d").alias("max_prior_unit_price_1d"),
    )
)

detail_rows = (
    per_group.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.sku_id",
        "s.prior_rows_1d",
        "s.first_moved_at",
        "s.total_prior_rows_1d",
        "s.max_prior_unit_price_1d",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
    )
)

result = (
    detail_rows
    .select(
        "sku_id",
        "prior_rows_1d",
        "first_moved_at",
        "total_prior_rows_1d",
        "max_prior_unit_price_1d",
        "move_id",
        "move_unit_price",
        "move_operator",
    )
    .orderBy("move_operator", F.col("sku_id").desc_nulls_first())
)
