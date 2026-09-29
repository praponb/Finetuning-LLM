from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id")
w2 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & F.col("supplier_id").isin([11, 12, 16, 35]))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.add_months("launch_date", 1).alias("launch_date_plus_months"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

result = (
    filtered
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "stock_qty",
        "launch_date",
        "launch_date_plus_months",
        F.lag("cost").over(w).alias("prev_cost"),
        F.lead("price").over(w).alias("next_price"),
        F.row_number().over(w).alias("seq_no"),
        F.first("cost", ignorenulls=True).over(w2).alias("first_known_cost"),
    )
)
