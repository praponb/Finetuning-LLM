from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_shipments
    .filter(F.year("shipped_date") == 2023)
    .select(
        "*",
        F.sum("cost").over(w_part).alias("running_sum_cost"),
        F.count("cost").over(w_part).alias("running_count_cost"),
        F.min("cost").over(w_part).alias("running_min_cost"),
        F.max("cost").over(w_part).alias("running_max_cost"),
    )
)

result = (
    scored
    .groupBy("carrier")
    .agg(
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_rows"),
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_rows_2"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .filter(F.col("dhl_rows_2") > 1)
    .orderBy(F.col("carrier").desc_nulls_last())
)
