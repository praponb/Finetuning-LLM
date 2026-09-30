from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .filter(F.col("store_id") == 2)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.sum("units").over(w_ordered).alias("running_sum_units"),
        F.count("units").over(w_ordered).alias("running_count_units"),
        F.min("units").over(w_ordered).alias("running_min_units"),
    )
)
