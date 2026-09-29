from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy(F.year("sale_date"))
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").asc_nulls_last())
w4 = Window.partitionBy("product_id", "store_id").orderBy("units", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.count("units").over(w_part).alias("sale_date_year_count_units"),
        F.count("revenue").over(w2).alias("product_id_count_revenue"),
        F.dense_rank().over(w3).alias("units_dense_rank"),
        F.row_number().over(w4).alias("event_seq"),
    )
)
