from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_city_by_capacity = Window.partitionBy("city").orderBy("capacity", "id")
w_city_capacity_seq_by_capacit = Window.partitionBy("city", "capacity_seq").orderBy("capacity", "id")

filtered = df_warehouses.filter(F.col("wh_code").isNotNull())

windowed = (
    filtered
    .select(
        "*",
        F.max("capacity").over(w_city).alias("city_max_capacity"),
        F.round(F.col("capacity") - F.avg("capacity").over(w_city), 2).alias("capacity_vs_city_avg"),
        F.row_number().over(w_city_by_capacity).alias("capacity_seq"),
        F.dense_rank().over(w_city_by_capacity).alias("capacity_dense_rank"),
        F.round(F.percent_rank().over(w_city_by_capacity), 3).alias("capacity_percent_rank"),
    )
)

leaders = (
    windowed
    .filter((F.col("capacity_dense_rank") <= 3) & (F.col("capacity_seq") > 5))
    .drop("capacity_dense_rank")
)

ranked = (
    leaders
    .withColumn("next3_capacity_seq", F.lead("capacity_seq", 3, 0).over(w_city_capacity_seq_by_capacit))
    .withColumn("capacity_vs_city_avg_2", F.round(F.col("capacity") - F.avg("capacity").over(w_city), 2))
)

result = (
    ranked
    .filter(F.col("capacity_percent_rank") >= 0.1)
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        "city_max_capacity",
        "capacity_vs_city_avg",
        "capacity_seq",
        "capacity_vs_city_avg_2",
        F.concat_ws("|", F.col("wh_code"), F.col("city")).alias("wh_code_city_label"),
    )
    .orderBy("opened_on", F.col("capacity_seq").asc())
)
