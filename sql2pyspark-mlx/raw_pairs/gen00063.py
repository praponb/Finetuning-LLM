from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("basket_amount").desc(), "id")

unpacked = (
    df_carts
    .filter(F.col("customer_id").isin([12, 33]))
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        F.col("attributes")["device"].alias("device"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)

result = (
    unpacked
    .withColumn("last3_count_bulk_lines", F.count("bulk_lines").over(w_part))
    .withColumn("last3_max_bulk_lines", F.max("bulk_lines").over(w_part))
    .withColumn("last3_sum_bulk_lines", F.sum("bulk_lines").over(w_part))
    .withColumn("seq_no", F.row_number().over(w2))
)
