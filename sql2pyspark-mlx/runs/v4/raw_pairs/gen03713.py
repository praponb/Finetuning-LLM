from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock_plus_months").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("seq_no").asc_nulls_first(), "id")

cleaned = (
    df_inventory
    .filter(F.col("product_id").isin([1, 6, 10]))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)

cleaned_inventory = (
    cleaned
    .filter(F.col("product_id").isin([1, 9, 10]))
    .select("*", F.add_months("last_restock", 6).alias("last_restock_plus_months"))
)

metrics = (
    cleaned_inventory
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked
    .select(
        "id",
        "last_restock",
        "last_restock_label",
        "last_restock_plus_months",
        "seq_no",
        "row_seq",
    )
    .orderBy(F.col("last_restock_plus_months").asc_nulls_first(), "last_restock")
)
