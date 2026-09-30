from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id", "product").orderBy(F.col("end_date").desc(), F.col("id").desc())

filtered = (
    df_policies
    .filter((F.col("product").isNotNull()) | (F.col("status").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
    )
)

unique_rows = (
    filtered
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .groupingSets(
        [["end_date_dayofweek", "holder_id", "product"], ["end_date_dayofweek"], []],
        "end_date_dayofweek", "holder_id", "product",
    )
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("n_rows"),
    )
    .select(
        "end_date_dayofweek",
        "holder_id",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "status_list",
        "n_rows",
    )
)
