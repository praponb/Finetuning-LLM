from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w3 = Window.orderBy("launch_date", "id")
w_hist = Window.orderBy(F.col("launch_date").desc(), "id")

deduped = (
    df_suppliers
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "supplier_name")
)

eligible = (
    deduped
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isNotNull()), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

metrics = (
    with_latest
    .select(
        "*",
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.row_number().over(w_hist).alias("seq_no_2"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.row_number().over(w_hist).alias("event_seq_2"),
    )
)

eligible_suppliers = (
    metrics
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date") <= F.col("s.launch_date")), "left_anti")
)

result = (
    eligible_suppliers
    .select("id", "supplier_name", "launch_date", "seq_no_2", "row_seq", "event_seq_2")
    .orderBy(F.col("launch_date").desc_nulls_last())
)
